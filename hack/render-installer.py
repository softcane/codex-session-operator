#!/usr/bin/env python3
"""Render distributable manifests without changing checked-in overlays."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def image_override(image):
    if not image or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/+:@-]*", image):
        raise ValueError("A container image reference is required")
    name, separator, digest = image.partition("@")
    if separator:
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ValueError("Image digest must be sha256 followed by 64 lowercase hex digits")
        return {"name": "controller", "newName": name, "digest": digest}
    repository, colon, tag = image.rpartition(":")
    if colon and "/" not in tag:
        return {"name": "controller", "newName": repository, "newTag": tag}
    return {"name": "controller", "newName": image}


def render(kustomize, image, overlay, executor_image=None):
    specification = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                     "resources": ["../" + overlay], "images": [image_override(image)]}
    if overlay not in ("default", "work"):
        raise ValueError("Only default and work release overlays are supported")
    if executor_image:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/+:\-]*@sha256:[a-f0-9]{64}", executor_image):
            raise ValueError("Executor image must be an immutable repository@sha256:digest")
        patch = {"apiVersion": "apps/v1", "kind": "Deployment",
                 "metadata": {"name": "controller-manager", "namespace": "system"},
                 "spec": {"template": {"spec": {"containers": [{"name": "manager", "env": [
                     {"name": "EXECUTOR_IMAGE", "value": executor_image},
                     {"name": "EXECUTOR_IMAGES", "value": executor_image}]}]}}}}
        specification["patches"] = [{"patch": json.dumps(patch)}]
    with tempfile.TemporaryDirectory(prefix=".installer-", dir=ROOT / "config") as temporary:
        directory = Path(temporary)
        (directory / "kustomization.yaml").write_text(json.dumps(specification))
        return subprocess.check_output([str(Path(kustomize).resolve()), "build", str(directory)], text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kustomize", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--executor-image")
    parser.add_argument("--overlay", choices=("default", "work"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        manifest = render(args.kustomize, args.image, args.overlay, args.executor_image)
    except ValueError as error:
        parser.error(str(error))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(manifest)


if __name__ == "__main__":
    main()

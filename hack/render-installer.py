#!/usr/bin/env python3
"""Render distributable manifests without changing checked-in overlays."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def image_override(logical_name, image):
    if not image or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/+:@-]*", image):
        raise ValueError("A container image reference is required")
    repository, separator, digest = image.partition("@")
    if separator:
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ValueError("Image digest must be sha256 followed by 64 lowercase hex digits")
        return {"name": logical_name, "newName": repository, "digest": digest}
    repository, colon, tag = image.rpartition(":")
    if colon and "/" not in tag:
        return {"name": logical_name, "newName": repository, "newTag": tag}
    return {"name": logical_name, "newName": image}


def render(kustomize, controller_image, gateway_image, sandbox_pools=None):
    specification = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                     "resources": ["../default"], "images": [
                         image_override("controller", controller_image),
                         image_override("gateway", gateway_image),
                     ]}
    if sandbox_pools:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?:,[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?)*", sandbox_pools):
            raise ValueError("Sandbox pools must be comma-separated Kubernetes names")
        patch = {"apiVersion": "apps/v1", "kind": "Deployment",
                 "metadata": {"name": "controller-manager", "namespace": "system"},
                 "spec": {"template": {"spec": {"containers": [{"name": "manager", "env": [
                     {"name": "SANDBOX_POOLS", "value": sandbox_pools}]}]}}}}
        specification["patches"] = [{"patch": json.dumps(patch)}]
    with tempfile.TemporaryDirectory(prefix=".installer-", dir=ROOT / "config") as temporary:
        directory = Path(temporary)
        (directory / "kustomization.yaml").write_text(json.dumps(specification))
        return subprocess.check_output([str(Path(kustomize).resolve()), "build", str(directory)], text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kustomize", required=True)
    parser.add_argument("--controller-image", required=True)
    parser.add_argument("--gateway-image", required=True)
    parser.add_argument("--sandbox-pools")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        manifest = render(args.kustomize, args.controller_image, args.gateway_image,
                          args.sandbox_pools)
    except ValueError as error:
        parser.error(str(error))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(manifest)


if __name__ == "__main__":
    main()

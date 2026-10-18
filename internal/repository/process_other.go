//go:build !linux && !darwin

package repository

import "os/exec"

func configureProcess(_ *exec.Cmd) {}

package main

import (
	"go.uber.org/fx"

	"sigs.k8s.io/ome/internal/ome-agent/enigma"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/vault/kmscrypto"
	"sigs.k8s.io/ome/pkg/vault/kmsmgm"
	"sigs.k8s.io/ome/pkg/vault/kmsvault"
	ocisecret "sigs.k8s.io/ome/pkg/vault/secret"
	ocivault "sigs.k8s.io/ome/pkg/vault/vault"
)

// enigmaCommand runs the enigma agent, which encrypts and decrypts model
// weights with keys held in a vault. main registers it beside the commands table.
var enigmaCommand = command{
	name:  "enigma",
	short: "Run OME Enigma Agent",
	long:  "OME Agent Enigma is dedicated for model encryption and decryption.",
	modules: []fx.Option{
		kmsvault.Module, kmscrypto.Module, kmsmgm.Module, ocisecret.Module, ocivault.Module,
		afero.Module, logging.Module, logging.ModuleNamed("another_log"), enigma.Module,
	},
	runs: fxBuilt[*enigma.Enigma](),
}

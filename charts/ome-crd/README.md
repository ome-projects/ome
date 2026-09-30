# ome-crd

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.16.0](https://img.shields.io/badge/AppVersion-1.16.0-informational?style=flat-square)

OME Custom Resource Definitions

## Generated templates

`make manifests` generates every file under `templates/ome.io_*.yaml` and
`templates/_schemas.tpl` from `config/crd/full`. Schema subtrees that repeat
across the CRDs live once in `_schemas.tpl` as named templates, which keeps the
Helm release record far below its 1 MiB limit; the rendered CRDs are identical
to `config/crd/full`, and `tests/render_test.sh` enforces that. Edit the Go API
types under `pkg/apis/ome/v1beta1` and rerun `make manifests`; never edit the
templates by hand. To apply the CRDs without Helm, use
`kubectl apply --server-side -k config/crd`.

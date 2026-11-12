package testing

// RDMAProfileFixtureJSON is the rdma block the test seeds carry. A chart
// install ships no RDMA profile (every cluster's fabric differs, so the
// per-cluster values file holds the table), and the tests that exercise
// the injection bring this table instead. Its three profiles are the three
// shapes an overlay writes: environment with the device and shared-memory
// volumes and the IPC_LOCK capability, environment alone, and Multus network
// attachments alone.
const RDMAProfileFixtureJSON = `{
  "defaultProfile": "roce",
  "profiles": {
    "roce": {
      "env": {
        "NCCL_DEBUG": "INFO",
        "NCCL_IB_HCA": "=mlx5_0,mlx5_1",
        "NCCL_IB_TC": "41",
        "NCCL_SOCKET_IFNAME": "eth0"
      },
      "volumeMounts": [
        {"name": "dshm", "mountPath": "/dev/shm"},
        {"name": "devinf", "mountPath": "/dev/infiniband"}
      ],
      "volumes": [
        {"name": "dshm", "emptyDir": {"medium": "Memory"}},
        {"name": "devinf", "hostPath": {"path": "/dev/infiniband"}}
      ],
      "securityContext": {"capabilities": {"add": ["IPC_LOCK"]}}
    },
    "collective-env": {
      "env": {
        "NCCL_IB_ADDR_FAMILY": "AF_INET6",
        "NCCL_SOCKET_IFNAME": "eth0"
      }
    },
    "multus-attachments": {
      "networks": [
        {"name": "rdma0-macvlan", "namespace": "multus-system"},
        {"name": "rdma1-macvlan", "namespace": "multus-system"}
      ]
    }
  }
}`

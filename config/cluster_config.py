cluster_config = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_size_gb": 100
        }
    },
    "gce_cluster_config": {
        "subnetwork_uri": "default",
        "internal_ip_only": True
    },
    "software_config": {
        "image_version": "2.2-debian12",
        "optional_components": ["JUPYTER"]
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 3600},
        "auto_delete_ttl": {"seconds": 14400}
    },
    "endpoint_config": {
        "enable_http_port_access": True
    }
}
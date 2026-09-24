def test_skeleton_imports() -> None:
    from anomaly_service import AnomalyService, ServiceConfig

    assert AnomalyService is not None
    assert ServiceConfig().max_points_per_batch > 0

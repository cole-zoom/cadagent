output "raw_bucket_name" {
  value = google_storage_bucket.raw.name
}

output "processed_bucket_name" {
  value = google_storage_bucket.processed.name
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}

output "artifact_registry_url" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.containers.repository_id}"
}

output "agent_api_url" {
  value = google_cloud_run_v2_service.agent_api.uri
}

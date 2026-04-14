terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------- GCS Buckets ----------

resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-raw"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = var.environment == "dev"

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

resource "google_storage_bucket" "processed" {
  name          = "${var.project_id}-processed"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = var.environment == "dev"

  uniform_bucket_level_access = true
}

# ---------- BigQuery Datasets ----------

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.region
  description = "Raw document metadata and extraction artifacts"
}

resource "google_bigquery_dataset" "stg" {
  dataset_id  = "stg"
  location    = var.region
  description = "Staging tables for parsed but not yet canonical data"
}

resource "google_bigquery_dataset" "cur" {
  dataset_id  = "cur"
  location    = var.region
  description = "Curated dimension and fact tables for agent queries"
}

resource "google_bigquery_dataset" "quality" {
  dataset_id  = "quality"
  location    = var.region
  description = "Data quality and review metadata"
}

# ---------- Service Account ----------

resource "google_service_account" "pipeline" {
  account_id   = "trace-pipeline"
  display_name = "Trace Pipeline Service Account"
}

resource "google_project_iam_member" "pipeline_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_raw_writer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_raw_reader" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_processed_writer" {
  bucket = google_storage_bucket.processed.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_processed_reader" {
  bucket = google_storage_bucket.processed.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# ---------- Artifact Registry ----------

resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "trace-ca"
  format        = "DOCKER"
  description   = "Container images for trace-ca pipeline services"
}

# ---------- Cloud Run Jobs ----------

resource "google_cloud_run_v2_job" "ingest" {
  name     = "ingest-${var.environment}"
  location = var.region

  template {
    template {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/trace-ca/ingest:latest"

        resources {
          limits = {
            memory = "1Gi"
            cpu    = "1"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCS_RAW_BUCKET"
          value = google_storage_bucket.raw.name
        }
        env {
          name  = "GCS_PROCESSED_BUCKET"
          value = google_storage_bucket.processed.name
        }
      }

      service_account = google_service_account.pipeline.email
      timeout         = "3600s"
    }
  }
}

resource "google_cloud_run_v2_job" "extract" {
  name     = "extract-${var.environment}"
  location = var.region

  template {
    template {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/trace-ca/extract:latest"

        resources {
          limits = {
            memory = "2Gi"
            cpu    = "1"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCS_RAW_BUCKET"
          value = google_storage_bucket.raw.name
        }
        env {
          name  = "GCS_PROCESSED_BUCKET"
          value = google_storage_bucket.processed.name
        }
      }

      service_account = google_service_account.pipeline.email
      timeout         = "3600s"
    }
  }
}

resource "google_cloud_run_v2_job" "normalize" {
  name     = "normalize-${var.environment}"
  location = var.region

  template {
    template {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/trace-ca/normalize:latest"

        resources {
          limits = {
            memory = "4Gi"
            cpu    = "2"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
      }

      service_account = google_service_account.pipeline.email
      timeout         = "3600s"
    }
  }
}

# ---------- Cloud Run Service (Agent API) ----------

resource "google_cloud_run_v2_service" "agent_api" {
  name     = "agent-api-${var.environment}"
  location = var.region

  template {
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/trace-ca/agent-api:latest"

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          memory = "2Gi"
          cpu    = "1"
        }
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }

    service_account = google_service_account.pipeline.email
  }
}

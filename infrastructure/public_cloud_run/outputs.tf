output "url" {
  value = google_cloud_run_service.cr_service.status[0].url
}

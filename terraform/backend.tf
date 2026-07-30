terraform {
  backend "s3" {
    bucket       = "failure-detector-tfstate-905418324528-us-east-1"
    key          = "failure-resolver/poc/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}

#!/bin/bash

# Usage: ./build_proxy.sh <repository-name>

set -e

# Check if repository name parameter is provided
if [ $# -eq 0 ]; then
    echo "Error: Please provide a repository name"
    echo "Usage: $0 <repository-name>"
    exit 1
fi

REPO_NAME=$1

# Get region from AWS configuration
AWS_REGION=$(aws configure get region)
if [ -z "$AWS_REGION" ]; then
    echo "Error: Unable to get region information from AWS configuration"
    echo "Please run 'aws configure' to set default region, or set AWS_REGION environment variable"
    exit 1
fi
echo "Using region: $AWS_REGION"

# Get AWS account ID
AWS_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text)
if [ -z "$AWS_ACCOUNT" ]; then
    echo "Error: Unable to get AWS account ID"
    exit 1
fi
echo "Using account ID: $AWS_ACCOUNT"

# Complete ECR repository URI
ECR_URI="${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
FULL_REPO_URI="${ECR_URI}/${REPO_NAME}"

echo "========================================="
echo "Starting deployment to ECR"
echo "Repository name: $REPO_NAME"
echo "Full URI: $FULL_REPO_URI"
echo "========================================="

# Step 1: Create ECR repository (if it doesn't exist)
echo "Step 1: Creating ECR repository..."
if aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$AWS_REGION" 2>/dev/null; then
    echo "Repository $REPO_NAME already exists, skipping creation"
else
    aws ecr create-repository --repository-name "$REPO_NAME" --region "$AWS_REGION"
    echo "Repository $REPO_NAME created successfully"
fi

# Step 2: Login to ECR
echo "Step 2: Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_URI"
echo "Login successful"

# Step 3: Build Docker image using Dockerfile from parent directory
echo "Step 3: Building Docker image using Dockerfile from ../..."
if [ -f ../Dockerfile ]; then
    sudo docker build --provenance=false -t "$REPO_NAME" -f ../Dockerfile ..
    echo "Image built successfully"
else
    echo "Error: Dockerfile not found in parent directory (../Dockerfile)"
    exit 1
fi

# Step 4: Tag image
echo "Step 4: Tagging image..."
sudo docker tag "${REPO_NAME}:latest" "${FULL_REPO_URI}:latest"
echo "Image tagged successfully"

# Step 5: Push image to ECR
echo "Step 5: Pushing image to ECR..."
sudo docker push "${FULL_REPO_URI}:latest"
echo "Image pushed successfully"

echo "========================================="
echo "Deployment completed!"
echo "Repository URI: ${FULL_REPO_URI}:latest"
echo "========================================="

#!/usr/bin/env python
"""
Simple test to check reconciliation data and API response format
"""
import os
import sys
import django
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.contrib.auth import get_user_model
from reconciliation.models import ReconciliationJob
from reconciliation.serializers import ReconciliationJobSerializer
from rest_framework.pagination import PageNumberPagination

User = get_user_model()

def check_reconciliation_data():
    print("=== Checking Reconciliation Data ===")
    
    # Get test user
    user = User.objects.filter(email__icontains='test').first()
    if not user:
        print("No test user found")
        return
    
    print(f"User: {user.email}")
    
    # Get reconciliation jobs
    jobs = ReconciliationJob.objects.filter(user=user).order_by('-created_at')
    print(f"Found {jobs.count()} reconciliation jobs")
    
    if jobs.count() == 0:
        print("No reconciliation jobs found - this explains why the list is empty!")
        return
    
    # Serialize the data like the API would
    serializer = ReconciliationJobSerializer(jobs, many=True)
    serialized_data = serializer.data
    
    print(f"\nSerialized data type: {type(serialized_data)}")
    print(f"Number of items: {len(serialized_data)}")
    
    # Show what the API response would look like with pagination
    print("\n=== Expected API Response Format (with pagination) ===")
    paginated_response = {
        "count": len(serialized_data),
        "next": None,
        "previous": None,
        "results": serialized_data
    }
    
    print(json.dumps(paginated_response, indent=2, default=str)[:1000] + "..." if len(str(paginated_response)) > 1000 else json.dumps(paginated_response, indent=2, default=str))
    
    # Show first job details
    if serialized_data:
        print(f"\n=== First Job Details ===")
        first_job = serialized_data[0]
        print(f"ID: {first_job.get('id')}")
        print(f"Name: {first_job.get('name')}")
        print(f"Status: {first_job.get('status')}")
        print(f"Source Dataset: {first_job.get('source_dataset_name')}")
        print(f"Target Dataset: {first_job.get('target_dataset_name')}")

if __name__ == '__main__':
    check_reconciliation_data()
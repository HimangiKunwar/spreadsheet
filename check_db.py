#!/usr/bin/env python
import os
import sys
import django

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from authentication.models import User
from reports.models import Report

print("=== CHECKING DATABASE ===")

# Check users
print("Users in database:")
users = User.objects.all()
for user in users:
    print(f"  - {user.email} (ID: {user.id})")

if not users.exists():
    print("No users found. Creating test user...")
    user = User.objects.create_user(
        email="test@example.com",
        password="testpass123",
        first_name="Test",
        last_name="User"
    )
    print(f"Created user: {user.email}")

# Check reports
print("\nReports in database:")
reports = Report.objects.all()
for report in reports:
    print(f"  - {report.name} (ID: {report.id}, User: {report.user.email})")

if not reports.exists():
    print("No reports found. Creating test report...")
    user = User.objects.first()
    if user:
        report = Report.objects.create(
            name="Test Report",
            description="Test report for download verification",
            user=user,
            status="completed"
        )
        print(f"Created report: {report.name} (ID: {report.id})")
    else:
        print("No user available to create report")

print("\n=== DATABASE CHECK COMPLETE ===")
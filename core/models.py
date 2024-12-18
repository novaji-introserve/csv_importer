from django.db import models
from django.utils import timezone

class ImportLog(models.Model):
    """Track CSV import operations with detailed status and metrics"""
    TABLE_CHOICES = [
        ('civil_servant', 'Civil Servant'),
        ('repayment', 'Repayment'),
        ('loan_details', 'Loan Details')
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed')
    ]
    
    file_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=50, choices=TABLE_CHOICES)
    total_records = models.IntegerField(default=0)
    successful_records = models.IntegerField(default=0)
    failed_records = models.IntegerField(default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)
    # created_by = models.IntegerField()  # User ID who initiated import

    class Meta:
        indexes = [
            models.Index(fields=['status', 'created_at']),
            models.Index(fields=['table_name', 'created_at'])
        ]

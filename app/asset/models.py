from django.db import models
import uuid

class Asset(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=30)
    symbol = models.CharField(max_length=30)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    change_1d = models.DecimalField(max_digits=10, decimal_places=2)
    change_1w = models.DecimalField(max_digits=10, decimal_places=2)

    def __str__(self):
        return self.name

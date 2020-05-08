from django.db import models

# Create your models here.
class HistoryJob(models.Model):
    job_id = models.CharField(max_length=128, unique=True, verbose_name="任务ID")
    job_user = models.CharField(max_length=128, verbose_name="提交任务的用户")
    create_date = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")

    def __str__(self):
        return self.job_id

    @classmethod
    def add_job(cls, job_id, job_user):
        cls.objects.create(job_id=job_id, job_user=job_user)

    @classmethod
    def get_all(cls):
        jobs = cls.objects.all().values('job_id')
        return jobs

    class Meta:
        ordering = ['-create_date']
        verbose_name = verbose_name_plural = '任务'

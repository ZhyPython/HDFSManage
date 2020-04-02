from django.db import models


# Create your models here.
class UserInfo(models.Model):
    username = models.CharField(max_length=128, unique=True, verbose_name="用户名")
    password = models.CharField(max_length=256, verbose_name="密码")
    create_date = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")

    def __str__(self):
        return self.username
   
    @classmethod
    def get_all(cls):
        users = cls.objects.all().values('username')
        return users
    
    @classmethod
    def get_user(cls, username):
        user = cls.objects.get(username=username)
        return user

    @classmethod
    def create_user(cls, username, password):
        cls.objects.create(username=username, password=password)
    
    class Meta:
        ordering = ['-create_date']
        verbose_name = verbose_name_plural = '用户'


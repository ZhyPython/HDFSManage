from django.db import models
import re


__all__ = ['UserInfo', 'RolePrivilege']

# Create your models here.
class UserInfo(models.Model):
    ACCEPTED = 0
    ACCEPT_NOT = -1
    ACCEPT_ITEM = (
        (ACCEPTED, '已通过审核'),
        (ACCEPT_NOT, '账号审核中')
    )

    username = models.CharField(max_length=128, unique=True, verbose_name="用户名")
    password = models.CharField(max_length=256, verbose_name="密码")
    accepted = models.IntegerField(default=ACCEPT_NOT, choices=ACCEPT_ITEM, verbose_name="审核标志")
    create_date = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")

    def __str__(self):
        return self.username
    
    def get_user_state(self):
        """获取当前用户的用户名，accepted状态，创建时间
        """
        result = {}
        result['username'] = self.username

        if self.accepted == 0:
            result['state'] = '已通过审核'
        else:
            result['state'] = '账号审核中'
        
        date = re.sub(r'.(\d+)$', '', str(self.create_date))
        date = re.sub(r'T', ' ', date)
        result['createDate'] = date
        return result
   
    @classmethod
    def user_exits(cls, username):
        """判断用户是否存在，返回True或False
        """
        flag = cls.objects.filter(username=username).exists()
        return flag

    @classmethod
    def get_all(cls):
        users = cls.objects.all()
        return users
    
    @classmethod
    def get_user(cls, username):
        user = cls.objects.get(username=username)
        return user

    @classmethod
    def create_user(cls, username, password, accepted=-1):
        obj = cls.objects.create(username=username, password=password, accepted=accepted)
        return obj
    
    @classmethod
    def modify_passwd(cls, username, password):
        cls.objects.filter(username=username).update(password=password)
    
    class Meta:
        ordering = ['-create_date']
        verbose_name = verbose_name_plural = '用户'


class RolePrivilege(models.Model):
    ROLE_ADMIN = 0
    ROLE_USER = 1
    ROLE_ITEM = (
        (ROLE_ADMIN, '管理员'),
        (ROLE_USER, '普通用户')
    )

    PRIVILEGE_HOLD = 0
    PRIVILEGE_NOT_HOLD = -1
    PRIVILEGE_ITEM = (
        (PRIVILEGE_HOLD, '具有该权限'),
        (PRIVILEGE_NOT_HOLD, '不具有该权限')
    )

    role_name = models.PositiveIntegerField(default=ROLE_USER, choices=ROLE_ITEM ,verbose_name="角色名")
    # 查看用户的权限
    view_users = models.IntegerField(default=PRIVILEGE_NOT_HOLD, choices=PRIVILEGE_ITEM, verbose_name="查看用户的权限")
    # 删除用户的权限
    delete_user = models.IntegerField(default=PRIVILEGE_NOT_HOLD, choices=PRIVILEGE_ITEM, verbose_name="删除用户的权限")
    # 启用和禁用用户的权限
    add_user = models.IntegerField(default=PRIVILEGE_NOT_HOLD, choices=PRIVILEGE_ITEM, verbose_name='将用户添加进系统')
    user = models.ManyToManyField(UserInfo, related_name="role",verbose_name="用户")

    @classmethod
    def get_role(cls, role_name):
        role = cls.objects.get(role_name=role_name)
        return role

    @classmethod
    def create_role(cls, role_name, view_users, delete_user, add_user):
        obj = cls.objects.create(role_name=role_name, view_users=view_users, delete_user=delete_user, add_user=add_user)
        return obj
    
    @classmethod
    def admin_role_exits(cls, role_name):
        """判断管理员角色是否存在，返回True或False
        """
        flag = cls.objects.filter(role_name=role_name).exists()
        return flag

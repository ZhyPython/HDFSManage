# from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view

from .models import UserInfo, RolePrivilege, admin_role, user_role
# from .serializers import UserInfoSerializer


@api_view(['POST'])
def login_view(request):
    """登录系统，创建session
    """
    # 返回的数据
    context = {}
    # 查询当前用户是否在数据库中
    try:
        user = UserInfo.get_user(request.POST['username'])
    except UserInfo.DoesNotExist:
        context.update({
            'info': '当前用户不存在'
        })
    else:
        # 先判断账号是否通过了管理员审核
        if user.accepted == -1:
            context.update({
                'info': '账号未通过管理员审核'
            })
            return Response(context)
        
        # 判断密码是否正确，密码正确则创建session
        if user.password == request.POST['password']:
            context.update({
                'info': 'success'
            })
            # 创建session
            request.session['username'] = request.POST['username']
            # session过期时间为一天
            request.session.set_expiry(24 * 60 * 60)
        else:
            context.update({
                'info': '密码错误'
            })
    return Response(context)

@api_view(['POST'])
def sign_up(request):
    """注册用户
    """
    context = {}
    username = request.POST['username']
    password = request.POST['password']
    # 判断注册用户名是否在数据库中
    exist_flag = UserInfo.user_exits(username)
    if exist_flag:
        context.update({
                'info': '用户已存在'
            })
        return Response(context)
    user = UserInfo.create_user(username, password)
    # 添加权限关系
    # print(user_role)
    user.role.add(user_role)
    context.update({
        'info': 'success'
    })
    return Response(context)

@api_view(['GET'])
def valid(request):
    """通过session判断是否需要登录,如果已经登录过，就返回标志和登录用户名
    """
    context = {}
    username = request.session.get('username', '')
    if not username:
        context.update({
            'info': 'not_login'
        })
    else:
        context.update({
            'info': 'logined',
            'username': username,
        })
    return Response(context)

@api_view(['GET'])
def delete_session(request):
    """删除session
    """
    # del会更改DB中的session_data的值，所以数据库中的记录还在，但是无法识别当前session，验证不会通过
    # del request.session['username']
    # flush会删除DB中的具体session记录
    request.session.flush()
    return Response()

@api_view(['POST'])
def modify_passwd(request):
    """修改密码
    """
    context = {}
    username = request.data['username']
    passwd = request.data['passwd']
    UserInfo.modify_passwd(username, passwd)
    return Response(context)

@api_view(['GET'])
def check_admin_user(request):
    """检查当前用户是否是管理员
    """
    context = {}
    current_username = request.GET['username']
    # 判断当前用户能否查看用户列表
    user = UserInfo.get_user(current_username)
    privilege_flag = user.role.filter(view_users=0).exists()
    if privilege_flag:
        context.update({
            'flag': 0
        })
    else:
        context.update({
            'flag': -1
        })
    return Response(context)


@api_view(['GET'])
def list_users(request):
    """查询所有的用户, 只有当前用户是管理员时才能查询用户
    """
    context = []
    current_username = request.GET['username']
    # 判断当前用户能否查看用户列表
    user = UserInfo.get_user(current_username)
    privilege_flag = user.role.filter(view_users=0).exists()
    if privilege_flag:
        # 当前用户有权限查看用户列表,返回的列表中排除自己
        users = UserInfo.get_all().exclude(username=current_username)
        # 查询用户的角色，添加角色到users中并返回
        for user in users:
            temp = user.get_user_state()
            role = user.role.all().values_list('role_name', flat=True)
            if role[0] == 0:
                temp.update({'role': '管理员'})
            elif role[0] == 1:
                temp.update({'role': '普通用户'})
            else:
                pass
            context.append(temp)
    return Response(context)


@api_view(['POST'])
def enable_user(request):
    """通过账号的审核，使之能够登录系统
    """
    context = {}
    # 传输的数据格式应该是列表
    current_user = request.data['currentUser']
    users = request.data['users']
    # 判断当前用户是否有权限审核账号
    current_user = UserInfo.get_user(current_user)
    enable_privilege = current_user.role.all().values_list('add_user', flat=True)
    if enable_privilege[0] == 0:
        # 有权限则对users中的用户进行审核开放
        for user in users:
            u = UserInfo.get_user(username=user['username'])
            u.accepted = 0
            u.save()
        context.update({
            'info': 'success'
        })
    else:
        context.update({
            'info': 'not_privilege'
        })
    return Response(context)

@api_view(['POST'])
def disable_user(request):
    """禁用账号
    """
    context = {}
    # 传输的数据格式应该是列表
    current_user = request.data['currentUser']
    users = request.data['users']
    # 判断当前用户是否有权限审核账号
    current_user = UserInfo.get_user(current_user)
    disable_privilege = current_user.role.all().values_list('add_user', flat=True)
    if disable_privilege[0] == 0:
        # 有权限则对users中的用户进行禁用
        for user in users:
            u = UserInfo.get_user(username=user['username'])
            u.accepted = -1
            u.save()
        context.update({
            'info': 'success'
        })
    else:
        context.update({
            'info': 'not_privilege'
        })
    return Response(context)

@api_view(['POST'])
def delete_user(request):
    """删除账号
    """
    context = {}
    # 传输的数据格式应该是列表
    current_user = request.data['currentUser']
    users = request.data['users']
    # 判断当前用户是否有权限删除账号
    current_user = UserInfo.get_user(current_user)
    delete_privilege = current_user.role.all().values_list('delete_user', flat=True)
    if delete_privilege[0] == 0:
        # 有权限则对删除用户
        for user in users:
            u = UserInfo.get_user(username=user['username'])
            # 删除用户，先用remove删除指定对象的关系（clear方法无参数，会删除所有关系），然后再delete删除数据库中的user对象
            u.role.remove(user_role)
            _ = u.delete()
        context.update({
            'info': 'success'
        })
    else:
        context.update({
            'info': 'not_privilege'
        })
    return Response(context)
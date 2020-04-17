# from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view

from .models import UserInfo
# from .serializers import UserInfoSerializer


def ssesion_wrapper(func):
    """session会话装饰器，登录之前检查session是否有当前用户
    """
    def wrapper(request):
        if request.session.get('username', False):
            pass
        else:
            # 创建session
            request.session['username'] = request.POST['username']
            # session过期时间为一天
            request.session.set_expiry(24 * 60 * 60)
        return func(request)
    return wrapper

@api_view(['POST'])
@ssesion_wrapper
def login_view(request):
    """登录系统，创建用户
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
        if user.password == request.POST['password']:
            context.update({
                'info': 'success'
            })
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
    # 获取所有用户名，确保注册用户名不在其中
    users = UserInfo.get_all()
    for user in users:
        print(user)
        if username == user['username']:
            context.update({
                'info': '用户已存在'
            })
            return Response(context)
    UserInfo.create_user(username, password)
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

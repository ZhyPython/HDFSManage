from django.urls import path
from .views import login_view, valid, delete_session, sign_up, \
                   list_users, enable_user, disable_user, delete_user, \
                   check_admin_user


urlpatterns = [
    path('login/', login_view, name="login"),
    path('valid/', valid, name="valid"),
    path('delete_session/', delete_session, name="delete_session"),
    path('signup/', sign_up, name="sign_up"),
    path('list_users/', list_users, name="list_users"),
    path('enable_user/', enable_user, name="enable_user"),
    path('disable_user/', disable_user, name="disable_user"),
    path('delete_user/', delete_user, name="delete_user"),
    path('check_admin_user/', check_admin_user, name="check_admin_user"),
]

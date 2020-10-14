import random
import hashlib
import string


def get_random_salt(length=8):
    """随机生成密码加密的盐值
    默认长度的为8，从大小写字母和数字中随机选取
    """
    salt = ''.join(random.sample(string.ascii_letters + string.digits, length))
    return salt

def hash_salt_pwd(pwd, salt):
    """给密码加盐值并进行加密
    返回值为加盐后哈希的密码
    """
    salt_pwd = salt + pwd
    sha256_pwd = hashlib.sha256(salt_pwd.encode()).hexdigest()
    return sha256_pwd


if __name__ == "__main__":
    print(hash_salt_pwd('a9abe05543b7763756d79a7172d6639978586847461e3813f99341d96c9308b7', get_random_salt()))

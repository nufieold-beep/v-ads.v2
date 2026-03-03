import paramiko
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('173.208.137.202', username='root', password='Dewa@123')
commands = [
    'cd /root/v-ads && git stash && git pull origin main',
    'cd /root/v-ads && docker compose up -d --build ad-server'
]
for cmd in commands:
    print(f"RUNNING: {cmd}")
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8')
    err = stderr.read().decode('utf-8')
    print("STDOUT:", out)
    if err:
        print("STDERR:", err)

client.close()
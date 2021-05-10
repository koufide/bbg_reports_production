import  configparser

config = configparser.ConfigParser()

print(config.sections())

config.read('config.ini')

print(config.sections())

print('koufide.com' in config)


print(config['koufide.com']['User'])

for key in (config['fidelinux.com']):
    print(key)


if(__name__ == "__main__"):
    print(__name__)
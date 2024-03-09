#FAULT TOLERANCE
import yaml
import subprocess

#WORD COUNT
#mappers = 3
#reducers = 2

with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

#make changes in config
cfg['application'] = 'word_count'
cfg['master']['mapperCount'] = 3
cfg['master']['reducerCount'] = 2
cfg['fault'] = 5

#update config
with open('config.yml', 'w') as f:
    yaml.dump(cfg, f)
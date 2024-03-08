import yaml
import subprocess

#WORD COUNT
#mappers = 5
#reducers = 3

with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

#make changes in config
cfg['application'] = 'word_count'
cfg['master']['mapperCount'] = 5
cfg['master']['reducerCount'] = 3

#update config
with open('config.yml', 'w') as f:
    yaml.dump(cfg, f)

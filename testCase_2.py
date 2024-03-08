import yaml
import subprocess

#INVERTED INDEX
#mappers = 3
#reducers = 2

with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

#make changes in config
cfg['application'] = 'inverted_index'
cfg['master']['mapperCount'] = 3
cfg['master']['reducerCount'] = 2

#update config
with open('config.yml', 'w') as f:
    yaml.dump(cfg, f)

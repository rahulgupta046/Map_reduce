import yaml
import subprocess

#INVERTED INDEX
#mappers = 5
#reducers = 3

with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

#make changes in config
cfg['application'] = 'inverted_index'
cfg['master']['mapperCount'] = 5
cfg['master']['reducerCount'] = 3

#update config
with open('config.yml', 'w') as f:
    yaml.dump(cfg, f)

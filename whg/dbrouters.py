from places.models import Place
from datasets.models import Dataset

class MyDBRouter(object):

    #
    def db_for_read(self, model, **hints):
        if model in (Place, Dataset):
            return 'whgdata'
        else:
            return 'default'
        return None

    def db_for_write(self, model, **hints):
        if model in (Place, Dataset):
            return 'whgdata'
        else:
            return 'default'
        return None

class InitLocker(type):
    def __call__(cls, *args, **kwargs):
        raise ValueError('Class cannot be created')

class ScopedResource(object):
    """This is a helper to make a class usable in a pattern like this:

        resource = ScopedResource(SomeClass)

        def use_resource():
          resource.method(stuff)  # calls SomeClass.method

        with resource(1, foo=2):
            use_resource()

    If a single keyword arg `instance` of type SomeClass is passed in,
    it will be used instead of creating a fresh instance on context
    manager entry:

        my_instance = SomeClass()

        with resource(instance=my_instance):
            use_resource()

    """
    def __init__(self, cls):
        self.cls = cls
        self.instance = None

    def __call__(self, *args, instance=None, **kwargs):
        self.args = args
        self.instance = instance
        self.kwargs = kwargs
        return self

    def __enter__(self):
        if not self.instance:
            self.instance = self.cls(*self.args, **self.kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        self.instance = None

    def __getattr__(self, name):
        return getattr(self.instance, name)

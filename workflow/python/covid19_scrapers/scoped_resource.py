class ScopedResource(object):
    """This is a helper to make a class usable in a pattern like this:

        resource = ScopedResource(SomeClass)

        def use_resource():
          resource.method(stuff)  # calls SomeClass.method

        with resource(1, foo=2):
            use_resource()

    If `with_instance` is used instead, its argument will be used
    instead of creating a fresh instance on context manager entry:

        my_instance = SomeClass()

        with resource.with_instance(instance):
            use_resource()

    """

    def __init__(self, cls):
        self.cls = cls
        self.instance = None

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    def with_instance(self, instance):
        assert isinstance(instance, self.cls), 'Invalid argument type'
        self.instance = instance
        return self

    def __enter__(self):
        if not self.instance:
            self.instance = self.cls(*self.args, **self.kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        self.instance = None

    def __getattr__(self, name):
        return getattr(self.instance, name)

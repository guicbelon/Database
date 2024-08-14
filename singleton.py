class Singleton(type):
    """
    A metaclass that implements the Singleton design pattern.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Overrides the __call__ method to ensure that only one instance of the class is created.

        If an instance of the class already exists, it returns that instance.
        Otherwise, it creates a new instance, stores it in the _instances dictionary, and returns it.

        Parameters
        ----------
        cls : type
            The class that is being instantiated.
        *args : tuple
            Positional arguments to pass to the class constructor.
        **kwargs : dict
            Keyword arguments to pass to the class constructor.

        Returns
        -------
        object
            The single instance of the class.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

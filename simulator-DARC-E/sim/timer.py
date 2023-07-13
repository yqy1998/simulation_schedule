#!/usr/bin/env python
"""Timer object for maintaining simulation time."""


class Timer:
    """Simulation clock
    Unit of time is ns.
    """
    elapsed = 0

    def increment(self, amount):
        """ Increment clock by specified amount. """
        self.elapsed += amount

    def get_time(self):
        """ Get current number of elapsed nanoseconds. """
        return self.elapsed

    def __str__(self):
        return "[TIME: {}]".format(self.elapsed)


# if __name__ == "__main__":
#     i = 0 
#     while i < 8:
#         time = Timer()
#         time.increment(100)
#         print(time.get_time())
    
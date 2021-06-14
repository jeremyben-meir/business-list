import time

class Counter:

  def __init__(self, length, precision=3): 
    self.length = length
    self.ticker = 0
    self.precision = precision
    self.start_time = time.time()
    self.beginning_time = 0
    print("Intantiated ticker with length " + str(self.length) + "...")

  def tick(self):
    if self.length == 0:
      raise ValueError('You ticked the counter but either did not initiate it, or set the counter length to zero.')
    if self.ticker == 0:
      self.beginning_time = time.time() - self.start_time
      print("Time to start: " + str(round(self.beginning_time)) + " s")
      self.start_time = time.time()
    self.ticker += 1
    if round(self.ticker/self.length,2+self.precision)>round((self.ticker-1)/self.length,2+self.precision):
      elapsed = time.time() - self.start_time
      print(str(round(100*self.ticker/self.length,self.precision)) + "% (" + str(round(elapsed)) + "s / " + str(round(elapsed*self.length/self.ticker))+ "s , IT: " + str(round(self.beginning_time)) + "s)")#, end='\r')
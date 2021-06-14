import time

class Counter:

  def __init__(self, length, precision=1): 
    self.length = length
    self.ticker = 0
    self.precision = precision
    self.start_time = time.time()

  def tick(self):
    if self.length == 0:
      raise ValueError('You ticked the counter but either did not initiate it, or set the counter length to zero.')
    if round(self.ticker/self.length,2+self.precision)>round((self.ticker-1)/self.length,2+self.precision):
      print(str(round(100*self.ticker/self.length,self.precision)) + "%")#, end='\r')
    self.ticker += 1        
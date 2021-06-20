import time

class ProgressMeter:
    
    def __init__(self, length, precision=1): 
        self.length = length
        self.ticker = 0
        self.precision = precision
        self.start_time = time.time()
        self.time_to_start = 0
        self.beginning_time = 0
        print(f"Instantiated ticker with length {self.length}...")

    def tick(self):
        if self.length == 0:
            raise ValueError('You ticked the counter but either did not initiate it, or set the counter length to zero.')
        if self.ticker == 0:
            self.beginning_time = time.time() - self.start_time
            self.time_to_start = round(self.beginning_time)
            print(f"Time to start: {self.time_to_start} s")
            self.start_time = time.time()
        self.ticker += 1
        if round(self.ticker/self.length,2+self.precision)>round((self.ticker-1)/self.length,2+self.precision):
            elapsed = round(time.time() - self.start_time)
            percent = round(100*self.ticker/self.length,self.precision)
            est_time = round(elapsed*self.length/self.ticker)
            print(f" {percent}%\t({elapsed}s / {est_time}s)", end='\r')
        if self.length <= self.ticker:
            print("\nDone!")

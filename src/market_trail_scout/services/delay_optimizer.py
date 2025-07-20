class DelayOptimizer:
    def __init__(self, initial_delay=2.0, max_delay=15.0, min_delay=1.0, tolerance=0.5):
        self.history = []  # stores tuples: (delay_before_request, download_duration, success_bool)
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.tolerance = tolerance

    def record_result(self, delay, duration, success):
        """Record each request's delay, duration, and success status."""
        self.history.append((delay, duration, success))
        if len(self.history) > 100:
            self.history.pop(0)

    def get_next_delay(self):
        """Determine the best delay to wait before the next request."""
        if not self.history:
            return 0.0  # Try immediately on the first-ever call

        # Separate out delays for successful and failed attempts
        successes = [d for d, _, s in self.history if s]
        failures = [d for d, _, s in self.history if not s]

        if not successes:
            # If nothing has succeeded yet, back off a little
            return min(self.max_delay, self.initial_delay * 1.5)

        # Average delay for successful requests
        avg_success_delay = sum(successes) / len(successes)

        # Shortest delay that led to failure
        worst_fail = min(failures) if failures else self.max_delay

        # How close is the edge of success to failure?
        safe_gap = worst_fail - avg_success_delay

        if safe_gap < self.tolerance:
            # They're too close — add buffer
            return min(self.max_delay, avg_success_delay + self.tolerance)
        else:
            # Use average delay that works
            return max(self.min_delay, avg_success_delay)

    def get_average_download_duration(self):
        """Get average time spent inside the actual download (not including delay)."""
        durations = [duration for _, duration, success in self.history if success]
        return sum(durations) / len(durations) if durations else 0.0

    def get_average_total_time(self):
        """Get average total time (delay + download) per successful request."""
        total_times = [delay + duration for delay, duration, success in self.history if success]
        return sum(total_times) / len(total_times) if total_times else 0.0

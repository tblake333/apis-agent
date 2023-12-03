class Version:
    def __init__(self, version: str):
        self.major, self.minor, self.patch = version.split(".")

    def __gt__(self, other):
        return self.major > other.major or (self.major == other.major and self.minor > other.minor) or (self.major == other.major and self.minor == other.minor and self.patch > other.patch)
    
    def __gte__(self, other):
        return self.major >= other.major or (self.major == other.major and self.minor >= other.minor) or (self.major == other.major and self.minor == other.minor and self.patch >= other.patch)
    
    def __lt__(self, other):
        return self.major < other.major or (self.major == other.major and self.minor < other.minor) or (self.major == other.major and self.minor == other.minor and self.patch < other.patch)
    
    def __lte__(self, other):
        return self.major <= other.major or (self.major == other.major and self.minor <= other.minor) or (self.major == other.major and self.minor == other.minor and self.patch <= other.patch)
    
    def __eq__(self, other):
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch
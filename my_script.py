
def doubles (input):
    return input*2


print(doubles(3))


class DataExtractor:

    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


data_extractor_object = DataExtractor(9)
print(data_extractor_object.get())
from metaflow import FlowSpec, step


class ShowGHAFlow(FlowSpec):
    @step
    def start(self):
        self.items = list(range(4))
        self.next(self.fanout, foreach="items")

    @step
    def fanout(self):
        self.result = f"item={self.input}"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted(inp.result for inp in inputs)
        self.next(self.end)

    @step
    def end(self):
        print("hello-from-gha")
        print(self.results)


if __name__ == "__main__":
    ShowGHAFlow()

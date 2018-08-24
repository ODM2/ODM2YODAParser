from yodatools.converter.Abstract import iOutputs
from yodatools.yodaparser.yamlPrinter import YamlPrinter


class yamlOutput(iOutputs):

    # def save(self, session, file_path):
    #     tables = self.parseObjects()
    #     data = []
    #     for t in tables:
    #         try:
    #             for o in session.query(t).all():
    #                 data.append(o)
    #                 Representer.add_representer(o, Representer.represent_name)  # noqa
    #         except Exception as e:
    #             print e
    #
    #
    #     with open(file_path, "w+") as f:
    #         f.write("test")
    #         yaml.safe_dump_all(data, f)
    #         #yaml.dump(data, f)
    #
    # def accept(self):
    #     raise NotImplementedError()

    def save(self, session, file_path):

        data = self.parseObjects(session)

        yp = YamlPrinter()
        yp.print_yoda(file_path, data)

    def accept(self):
        raise NotImplementedError()

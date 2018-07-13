from yodatools.dataloader.view.WizardDatabasePageView import WizardDatabasePageView


class WizardDatabasePageController(WizardDatabasePageView):
    def __init__(self, parent, title=''):
        super(WizardDatabasePageController, self).__init__(parent)

        del self.panel.choices['SQLite']
        self.panel.cbDatabaseType.SetItems(self.panel.choices.keys())
        self.panel.cbDatabaseType.SetStringSelection('MySQL')


        self.title = title

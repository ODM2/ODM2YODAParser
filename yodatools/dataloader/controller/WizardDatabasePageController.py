from yodatools.dataloader.view.WizardDatabasePageView import WizardDatabasePageView
import os


class WizardDatabasePageController(WizardDatabasePageView):
    def __init__(self, parent, title=''):
        super(WizardDatabasePageController, self).__init__(parent)

        del self.panel.choices['SQLite']
        self.panel.cbDatabaseType.SetItems(self.panel.choices.keys())

        self.panel.cbDatabaseType.SetStringSelection(os.getenv('DB_ENGINE', ''))
        self.panel.txtServer.SetValue(os.getenv('DB_HOST', ''))
        self.panel.txtUser.SetValue(os.getenv('DB_USER', ''))
        self.panel.txtDBName.SetValue(os.getenv('DB_NAME', ''))
        self.panel.txtPass.SetValue(os.getenv('DB_PWORD', ''))

        self.title = title

from yodatools.dataloader.view.WizardDatabasePageView import WizardDatabasePageView
import os


class WizardDatabasePageController(WizardDatabasePageView):
    def __init__(self, parent, title=''):
        super(WizardDatabasePageController, self).__init__(parent)

        del self.panel.choices['SQLite']
        self.panel.cbDatabaseType.SetItems(self.panel.choices.keys())

        if os.getenv('DB_ENGINE', None):
            self.panel.cbDatabaseType.SetStringSelection(os.getenv('DB_ENGINE'))
        if os.getenv('DB_HOST', None):
            self.panel.txtServer.SetValue(os.getenv('DB_HOST'))
        if os.getenv('DB_USER', None):
            self.panel.txtUser.SetValue(os.getenv('DB_USER'))
        if os.getenv('DB_NAME', None):
            self.panel.txtDBName.SetValue(os.getenv('DB_NAME'))
        if os.getenv('DB_PWORD', None):
            self.panel.txtPass.SetValue(os.getenv('DB_PWORD'))


        self.title = title

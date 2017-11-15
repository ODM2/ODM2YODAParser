# import wx
from wx import FileDialog, FD_SAVE, ID_OK
#
from yodatools.dataloader.view.WizardYodaPageView import WizardYodaPageView


class WizardYodaPageController(WizardYodaPageView):
    def __init__(self, parent, title=''):
        super(WizardYodaPageController, self).__init__(parent)
        self.title = title

    def on_browse_button(self, event):
        # dialog = FileDialog(
        #     self,
        #     message='Save to...',
        #     style=DD_CHANGE_DIR
        # )
        dialog = FileDialog(
            self,
            'YAML Output file',
            wildcard="YAML File (*.yaml)|*.yaml",
            style=FD_SAVE
        )

        if dialog.ShowModal() != ID_OK:
            return

        self.file_text_ctrl.SetValue(dialog.GetPath())

import wx

from yodatools.dataloader.view.WizardYodaPageView import WizardYodaPageView


class WizardYodaPageController(WizardYodaPageView):
    def __init__(self, parent, title=''):
        super(WizardYodaPageController, self).__init__(parent)
        self.title = title

    def on_browse_button(self, event):
        # dialog = wx.FileDialog(
        #     self,
        #     message='Save to...',
        #     style=wx.DD_CHANGE_DIR
        # )
        dialog = wx.FileDialog(
            self,
            'YAML Output file',
            wildcard="YAML File (*.yaml)|*.yaml",
            style=wx.FD_SAVE
        )

        if dialog.ShowModal() != wx.ID_OK:
            return

        self.file_text_ctrl.SetValue(dialog.GetPath())

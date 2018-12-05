# Sina-Warrant-Tracker
Real time tracker of warrants linked to HSI or HSCEI.

## Example

### Start Tracking
u = UpdateList([20360, 22781, 22078], "HSCEI")

### Refresh the real-time data
u.refresh()

### Output in list format
u.refresh_std()

## Source
Sina Stock API: real time warrant data

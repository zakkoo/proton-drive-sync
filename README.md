# Proton Drive Sync

![The Proton Drive Sync details page and bar panel during a sync](proton-drive-1080p.png)

One folder on your machine. The same files in Proton Drive. Proton Drive Sync keeps both sides current. Create, edit, rename, or move on either side, and the other side follows. Delete a file on your machine and it goes to a recycle folder. Delete it in Proton Drive and it goes to Trash. A large delete or replace waits for you. Change the same file on both sides, and you keep both copies.

You sign in through Proton's official [Drive SDK](https://github.com/ProtonDriveApps/sdk). The account code in this repository is a Node port of that SDK. The two-way sync on top of it is this project's own code.

Proton does not ship a Linux sync client yet. Until it does, you run this unofficial plugin on Omarchy. It is not affiliated with Proton AG or the Omarchy project.

## Install

```sh
omarchy plugin add https://github.com/zakkoo/proton-drive-sync.git --enable
```

## Sign in

Click the chip. Choose Sign in. A terminal opens Proton's own page, and your password stays there. Then name your local folder and your remote folder, for example `/my-files`, and confirm.


## Update

```sh
omarchy plugin update io.github.zakkoo.proton-drive
```

## Remove

```sh
omarchy plugin remove io.github.zakkoo.proton-drive
```

`omarchy plugin remove` takes the chip off your bar and stops the engine with it. It does not delete your sync folder, your Proton session, or the tool's config. Your files stay.

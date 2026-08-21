# medb
MeDB JSON Database

MeDB syncs regular files and attempts to sync directories. Filesystems that
report directory syncing as unsupported are accepted in best-effort mode;
strong power-loss durability for newly created or renamed files cannot be
guaranteed there. Other directory-sync failures return `ErrDirSync`.

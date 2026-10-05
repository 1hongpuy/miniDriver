#pragma once

#include "qtclient/TransferTypes.hpp"

#include <QString>
#include <QVector>

namespace miniKV::qtclient {

// Local task journal only.  Authoritative chunk completion remains in
// MetadataService; this database maps a stable commandId back to a local file.
class TransferStore final {
public:
    TransferStore();
    ~TransferStore();

    bool available() const noexcept { return available_; }
    QString error() const { return error_; }
    // Kept visible to the UI so an unavailable QSQLITE plugin or a permission
    // failure cannot silently disable crash recovery.
    QString databasePath() const { return databasePath_; }
    bool upsert(const TransferSpec& spec, const TransferSnapshot& snapshot);
    QVector<TransferSpec> interruptedUploads();

private:
    QString connectionName_;
    QString databasePath_;
    bool available_ = false;
    QString error_;
};

}  // namespace miniKV::qtclient

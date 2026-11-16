#pragma once
namespace coworkerd {
// OS-trusted operations tool only. No tenant-facing control-plane route.
int runDatabaseSeedRegister(int argc, char** argv);
}

#ifndef UA_API_HELPER_H
#define UA_API_HELPER_H

#include <string>

void apiInit(const std::string &apiserverHost, const int apiserverPort, const std::string &apiserverProtocol, const std::string &authToken);

void testServerConnection(void);

std::string resolveProject(const std::string &projectSpec);

void testProjectPermissions(const std::string &projectID);

void createFolder(const std::string &projectID, const std::string &folder);

std::string createFileObject(const std::string &project, const std::string &folder, const std::string &name);

void closeFileObject(const std::string &fileID);

std::string getFileState(const std::string &fileID);

#endif

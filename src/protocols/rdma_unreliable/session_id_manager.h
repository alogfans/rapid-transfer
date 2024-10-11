// Copyright 2024 Feng Ren

#ifndef SESSION_ID_MANAGER_H_
#define SESSION_ID_MANAGER_H_

#include "concurrency.h"

#include <map>

namespace rapid
{
    struct SessionIdManager
    {
    public:
        SessionIdManager() : next_sid_(0) {}

        int allocateSidByReceiver(const std::string &sender)
        {
            RWSpinlock::WriteGuard guard(session_lock_);
            int sid = next_sid_++;
            sid_to_sender_[sid] = sender;
            sender_to_sid_[sender] = sid;
            return sid;
        }

        void setSidBySender(const std::string &receiver, int sid)
        {
            RWSpinlock::WriteGuard guard(session_lock_);
            if (receiver_to_sid_.count(receiver) && receiver_to_sid_[receiver] != sid)
                LOG(ERROR) << "Session id has been assigned to different peers";
            receiver_to_sid_[receiver] = sid;
        }

        std::string getEndPointByReceiver(int sid)
        {
            RWSpinlock::ReadGuard guard(session_lock_);
            if (sid_to_sender_.count(sid))
                return sid_to_sender_[sid];
            return "";
        }

        int getSidBySender(const std::string &receiver)
        {
            RWSpinlock::ReadGuard guard(session_lock_);
            if (receiver_to_sid_.count(receiver))
                return receiver_to_sid_[receiver];
            return -1;
        }

        int getSidByReceiver(const std::string &sender)
        {
            RWSpinlock::ReadGuard guard(session_lock_);
            if (sender_to_sid_.count(sender))
                return sender_to_sid_[sender];
            return -1;
        }

    private:
        RWSpinlock session_lock_;
        int next_sid_;
        std::map<int, std::string> sid_to_sender_;
        std::map<std::string, int> sender_to_sid_;
        std::map<std::string, int> receiver_to_sid_;
    };
}

#endif // SESSION_ID_MANAGER_H_

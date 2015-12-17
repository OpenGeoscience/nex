#!/bin/bash

SESSION=NEX
i=0

run_command ()
{
    tmux list-windows -t $SESSION|cut -d: -f1|xargs -I{} tmux send-keys -t $SESSION:{} "$*" C-m
}

tmux new-session -d -s $SESSION 2>/dev/null
if [ $? -eq 0 ]; then

    for ip in $(head -n2 ip.list); do
        if [ "$i" -ne "0" ]; then
            tmux new-window -t$SESSION:$i
        fi
        tmux send-keys -t $SESSION:$i 'ssh -o StrictHostKeyChecking=no -o ControlPersist=4h ubuntu@'$ip  C-m
        tmux send-keys -t $SESSION:$i "tmux new-session -s import || tmux attach -t import" C-m
        ((i+=1))
    done
    tmux attach -t NEX
else
    if [ "$#" -gt 0 ]; then
        run_command $*
    fi
fi

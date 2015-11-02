#!/bin/bash

SESSION_DEFAULT=NEX
SESSION=${1-SESSION_DEFAULT}

CMD_DEFAULT=""
CMD=${2-CMD_DEFAULT}

KEYPATH=../keys/aws-NEX.pem
i=0

tmux kill-session -t NEX
tmux new-session -d -s NEX

for ip in $(aws --profile NEX \
                ec2 describe-instances  \
                --query Reservations[*].Instances[*].PublicIpAddress[] \
                   | grep "^    \"" \
                   | sed -e "s/\"//g" \
                   | sed -e "s/,//g"); do

    if [ "$i" -ne "0" ]; then
        tmux new-window -tNEX:$i
    fi

    tmux send-keys -t NEX:$i 'ssh -o StrictHostKeyChecking=no -i '$KEYPATH' ubuntu@'$ip  C-m
    tmux rename-window $ip
    tmux send-keys -t NEX:$i $CMD  C-m

    ((i+=1))

done

tmux attach -t NEX

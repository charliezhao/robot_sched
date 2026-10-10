# robot_sched

How to run it:
python main.py --demonstrate-resume

Run test:
pytest -v


Take-Home: A Queue for Robots That Get Stuck
In one sentence: build the queue and scheduler that drives tens of thousands of jobs across ten robot stations, where a station holds physical state, work assumes the table is in a particular arrangement, and a failure can need a human before anything else runs there.

Time: ~5 hours. You will not finish everything; scoping down deliberately is part of the exercise. AI tools are allowed and encouraged.


What this is
We run large batches of work on real arms, and a scheduler decides what runs where.

You are building that scheduler on your laptop. There is no robot and you do not need one. A station is a state machine with a timer: work occupies it for some seconds and then succeeds or fails. Nothing here needs physics and nothing needs a model. The problem is entirely in the scheduling and the bookkeeping.

Because a move is only a timer, you can run the clock as fast as you like (for simulating this).
The stations
A station is one table with two arms. There are ten. It runs one thing at a time, and it holds a physical state: the objects on the table, in some arrangement.

An operator is a human. There are two for all ten stations. They set tables up, and they deal with a table left in a state nobody can trust. They are the only way to physically move something that ended up somewhere it shouldn't.

An inference slot is a share of a GPU server. There are a few, shared across all ten stations, and anything running a model needs one.
The work
Four kinds, and they are not the same shape as each other. All of them compete for the same ten stations.

Trajectory replay. Pre-generated trajectories played back open-loop, the arm is replaying recorded joint positions, not looking at the table. Each one was generated assuming the table starts in a particular arrangement. Bulk throughput work, and most of the volume.
Policy inference. A policy checkpoint drives the arm in a closed loop. Needs an inference slot as well as a station, and how long it runs is not known up front.
World-model inference, in shadow. State and intended action are streamed to a server, which compares its prediction of the next state against what actually happened. It does not occupy a station of its own, it rides along on whatever is already running there, and must not perturb it.
Evals from checkpoints. A batch of episodes run against one checkpoint to score it. Someone is waiting on the result, and a batch that is 80% finished is worth nothing.

Some jobs refuse to start: work can carry a flag saying it is unsafe to play, and the queue honours that before the arm moves, recording it as something other than a failure.
Where replay comes from, and the question in it
Cubes sit on a grid on the table, and a move takes one from one cell to another. The task is self-resetting: after any move the table is still a valid arrangement for some other move, so an arm can keep going without a human.

We take real recorded episodes and generate large numbers of variants - same starting arrangement, different action, and play them on real arms to find out what actually happens. That is the ground truth we check a world model's predictions against.

Today these are queued as chains: an ordered run of around six moves, where move N's trajectory assumes the arrangement move N−1 was supposed to produce. Run strictly, that means a failure at move 3 makes moves 4 through 6 unrunnable, and an operator has to restore the table before the chain can continue.

Whether strict chains are the right model is part of what you are deciding. The trajectory needs the table to be in a particular arrangement. It does not inherently care how it got there. What that buys you, what it would require you to know about a table, and what it costs to find that out, is yours to work through.
The overhead between moves
When a move ends the arm is wherever it finished, which is not where the next trajectory starts, so it has to travel there. Separately, the gripper drifts and needs re-zeroing. Together that is around 40 s.

Neither has to happen every move, recalibrating every tenth is an option. But skipping it lets drift accumulate, which raises the failure rate, and failures cost operator time. There is an optimum and it is not "as rarely as possible."
The numbers
Real, from a recent run:





replay work queued
~4,840 chains of ~6 moves (29,040 moves)
duration of one move
~31 s
move failure rate
~50%
return-home + recalibration
~40 s
stations
10
operators
2


Pick your own numbers for inference slots, eval batch size, operator time per intervention, and the mix of the four kinds of work, and say why.

Do the arithmetic before you design anything. Where the time goes is not obvious, one resource dominates by a wide margin, and which one it is decides what your scheduler should be optimising for. Getting this wrong makes everything downstream the wrong shape.
Metadata
Keep the per-item metadata as small as you can get away with. For every field you define, be able to name the scheduling decision that needs it. If nothing branches on a field, it does not belong in the queue. Working out what the minimum actually is, is part of the task.
Where to start
Roughly the first hour, if it helps:

Ten stations, a flat queue of independent jobs, random failures. Get work flowing and finishing.
Now make replay work depend on the arrangement the table is in, so a job can only run where the table matches what it assumes. This is where the design starts being load-bearing.
Then the operator pool, then the other three kinds of work, then the scheduler decides rather than takes the next item.
Phases
1: A design note, no code. Decide the structure before you build it.

What the arithmetic says the bottleneck is, and therefore what the scheduler is for.
The states a station can be in, and what moves it between them.
How replay work relates to table state. A strict sequence, where a failure ends the run and needs a human? Or something looser, where what matters is whether the table currently matches what a job assumes? What each model costs you, what it requires you to know about a table, and how you would find that out. 
What happens when a move fails, under whichever model you chose. What does an operator actually have to do, and can the state they leave behind be trusted?
How four kinds of work with four different shapes share one queue: one with a long ordered run of dependent steps, one needing a second scarce resource, one attaching to work already running, one worth nothing until complete.
How often to recalibrate, given that the saving and the cost land in different places.
The minimum metadata, with the decision each field serves.
Dedup and retry semantics: the same job submitted twice, and a retry that has to be reproducible.
What the run persists so that killing the scheduler loses nothing and double-runs nothing.
What you are not building.

2: The engine and the scheduler.

Ten stations running all four kinds of work, with table state explicit, operators and inference slots contended, and preflight refusals recorded separately from failures.
Replay work gated on table state, however you decided to model that.
Failures injected at a rate you set, including part-way through a dependent run.
Resume: kill it mid-run, restart, and account for everything that was in flight.
A scheduler that decides, rather than one that pops the next item.
Deliverable: runnable code, a run over a meaningful slice of the work, and the resume demonstrated.

3: Make it watchable, and make it operable.

A fleet view you can put in front of someone and run: stations and what each is doing, the operator queue, inference slots in use, progress across the whole batch, eval batches and how close they are to being worth something, failures and refusals by reason, and a projected finish.
A per-station panel you can act from - what is running there now, what is queued behind it, why it is waiting if it is waiting, and the interventions an operator needs: pause the station, abort or requeue work, mark a table restored, take it out of service. Someone reaching in while the scheduler is mid-decision is the interesting part; say what makes that safe.
Run the clock fast enough that a multi-day schedule plays out while someone is watching.
Two approaches compared in it, same work and same seed, with the difference visible rather than described. If you took a position on the table-state question, the comparison worth showing is against the alternative (in operator hours, not just wall clock).
Deliverable: the view, the run behind it, and the comparison.

4: One extension, optional. 
a finish-time estimate that stays honest when the failure rate is not what you assumed and drifts per station
making the scheduler's decisions reconstructible after the fact, so you can ask why one job waited nine hours
per-station reliability as a first-class input, so a station that fails twice as often gets used differently
an API, and what it does when the queue is 30,000 deep and someone submits 5,000 more.
Submit
Code, runnable, short README. Assume it gets read rather than run.

DESIGN.md: your Phase 1 note, then per phase what you built and why. Include the arithmetic and what it told you, your position on how replay work relates to table state and what it costs, what an operator has to do on a failure, the station state machine, your metadata and the decision each field serves, what you persist for resume, what makes an outside intervention safe, the comparison with numbers, and what you cut and why.




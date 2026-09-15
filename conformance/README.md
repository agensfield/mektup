# Mektup v1 conformance scenarios (spec revision 1.0.3)

`v1/state-transitions.json` is the language-neutral delivery and reply
evidence state machine. Its forbidden transitions are part of the contract:
unknown outcomes never become rejection or retry authority, and an expired
reply claim never authorizes redispatch.

`v1/scenarios.json` maps the locked acceptance matrices to observable profiles.
Implementations can execute each scenario against a fake transport, isolated
app-server, or live acceptance target and report the profile's terminal event,
exit class, and listed observables. Fixture/schema validity is not live
app-server or physical acceptance.

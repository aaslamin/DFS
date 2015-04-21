JFLAGS = -g
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	ClientServerProtocol.java \
	Pinger.java \
	FileServer.java \
	LogRecord.java \
	RequestMessage.java \
	ServerMessage.java \
	Transaction.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
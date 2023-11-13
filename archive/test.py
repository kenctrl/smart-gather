import erdantic as erd
from erdantic.examples.pydantic import Party

erd.draw(Party, out="diagram.png")